from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(-2, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("unknown")).alias("utm_source_or_default"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    parsed
    .select(
        "id",
        "coupon_codes",
        "utm_source_or_default",
        F.max("bulk_lines").over(w).alias("last3_max_bulk_lines"),
        F.avg("bulk_lines").over(w).alias("last3_avg_bulk_lines"),
        F.sum("bulk_lines").over(w).alias("last3_sum_bulk_lines"),
    )
)
