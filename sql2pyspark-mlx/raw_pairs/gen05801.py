from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy(F.col("shipping_fee").desc(), "id")
w2 = Window.partitionBy("order_id", "carrier").orderBy(F.col("rn").desc(), "id")

filtered = (
    df_shipments
    .filter(F.col("delivered_date") > F.lit("2023-06-01").cast("date"))
    .select("*", F.date_sub("shipped_date", 33).alias("shipped_date_minus_33d"))
)

with_dims = (
    filtered
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.shipping_fee"))
)

ranked = (
    with_dims
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked_shipments = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "shipped_date", "cost", "status")
)

result = (
    ranked_shipments
    .select("id", "status")
    .orderBy(F.col("id").asc_nulls_first(), "status")
)
