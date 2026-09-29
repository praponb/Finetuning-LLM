from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc())

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .select(
        "*",
        F.when(F.col("carrier").isin(["Kerry", "Flash"]), "Tier 1").otherwise("Other").alias("carrier_group"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)
