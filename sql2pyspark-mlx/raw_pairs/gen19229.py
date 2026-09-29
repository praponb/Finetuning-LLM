from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.to_date("created_at")).orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("created_at"), "customer_id").orderBy("created_at", "id")

parsed = (
    df_carts
    .filter(F.col("customer_id").isin([6, 19, 20]))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "attributes",
        F.coalesce(F.col("attributes")["utm_source"], F.lit("direct")).alias("utm_source_or_default"),
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
    )
)

result = (
    parsed
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "utm_source_or_default",
        F.first("utm_source_or_default", ignorenulls=True).over(w).alias("first_known_utm_source_or_default"),
        F.first("utm_source_or_default", ignorenulls=True).over(w2).alias("first_known_utm_source_or_default_2"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
