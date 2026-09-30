from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("created_at"), "attr_key").orderBy("created_at", "id", "attr_key")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "attr_key",
        F.row_number().over(w).alias("event_seq"),
    )
)
