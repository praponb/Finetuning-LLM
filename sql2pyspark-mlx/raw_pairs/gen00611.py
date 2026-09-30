from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("val_tier").orderBy(F.col("starter_val").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("val") >= 61, "LARGE").otherwise("SMALL").alias("val_tier"),
        "plan",
    )
    .agg(
        F.sum("val").alias("total_val"),
        F.sum(F.when(F.col("plan") == "starter", F.col("val")).otherwise(0)).alias("starter_val"),
    )
    .withColumn("starter_val_rank", F.rank().over(w))
    .orderBy("val_tier")
)
