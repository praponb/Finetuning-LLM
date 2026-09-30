from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        "attributes",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.array_contains("coupon_codes", "BUNDLE").alias("used_bundle"),
    )
)

result = (
    parsed
    .filter(F.to_date("created_at") < "2025-02-19")
    .select(
        "id",
        "created_at",
        "utm_source",
        "used_bundle",
        F.row_number().over(w).alias("event_seq"),
        F.last("utm_source", ignorenulls=True).over(w2).alias("last_known_utm_source"),
        F.first("utm_source", ignorenulls=True).over(w3).alias("first_known_utm_source"),
    )
)
