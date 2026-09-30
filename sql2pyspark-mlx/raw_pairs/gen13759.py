from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost_filled").desc(), "id")

filtered = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-02-23"))) & (F.col("cost") < 34))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "shipped_date", "cost", "cost_filled")
)
