from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "created_at"), "customer_id").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "k",
        "attr_value",
        F.last("k", ignorenulls=True).over(w).alias("last_known_k"),
        F.first("k", ignorenulls=True).over(w2).alias("first_known_k"),
        F.first("k", ignorenulls=True).over(w3).alias("first_known_k_2"),
        F.last("k", ignorenulls=True).over(w2).alias("last_known_k_2"),
    )
)
