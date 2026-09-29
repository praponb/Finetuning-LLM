from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date")

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("shipped_date") > F.lit("2023-05-28").cast("date")))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.when(F.col("carrier") == "Flash", 1).otherwise(0).alias("is_flash"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

result = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "delivered_date", "cost", "carrier_filled")
)
