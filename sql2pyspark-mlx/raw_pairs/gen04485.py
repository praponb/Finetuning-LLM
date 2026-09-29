from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy(F.date_trunc("WEEK", "created_at")).orderBy(F.col("created_at").desc(), "id", "attr_key")
w2 = Window.partitionBy(F.year("created_at"), "customer_id").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("customer_id").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .filter(F.col("created_at") >= "2025-09-01")
    .select(
        "id",
        "attr_key",
        "attr_value",
        F.row_number().over(w_ordered).alias("row_seq"),
        F.last("attr_key", ignorenulls=True).over(w2).alias("last_known_attr_key"),
        F.first("attr_key", ignorenulls=True).over(w_recent).alias("first_known_attr_key"),
    )
)
