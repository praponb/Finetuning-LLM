from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_last(), "id")

filtered = (
    df_shipments
    .filter(F.col("delivered_date") <= "2023-05-01")
    .select(
        "*",
        F.floor("cost").alias("cost_whole"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "cost")
)
