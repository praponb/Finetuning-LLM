from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("attr_key", "customer_id").orderBy("created_at", "id", "attr_key")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

scored = (
    exploded
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("last_known_attr_key", F.last("attr_key", ignorenulls=True).over(w2))
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attr_key",
        "last_known_attr_key",
        "latest_rank",
    )
)
