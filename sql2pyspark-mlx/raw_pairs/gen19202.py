from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").asc_nulls_first())
w_part = Window.partitionBy("product_id").orderBy("quantity", "id")
w4 = Window.orderBy("quantity", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("product_id").orderBy("unit_price_avg_3_prior", "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest = (
    deduped
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    latest
    .filter(F.col("quantity") < 4)
    .select(
        "id",
        "product_id",
        F.lag("unit_price").over(w_part).alias("prev_unit_price"),
        F.lead("quantity").over(w_part).alias("next_quantity"),
        F.row_number().over(w_part).alias("event_seq"),
        F.avg("unit_price").over(w4).alias("unit_price_avg_3_prior"),
    )
)

latest_order_items = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest_order_items
    .select(
        "id",
        "product_id",
        "next_quantity",
        "unit_price_avg_3_prior",
        "latest_rank",
    )
    .orderBy(F.col("next_quantity").asc_nulls_first())
)
