from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

result = flattened.withColumn("val_percent_rank", F.percent_rank().over(w))
