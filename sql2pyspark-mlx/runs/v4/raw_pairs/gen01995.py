from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("created_at", "id", "attr_key")

lines = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("attr_key", "value"))
)

result = (
    lines
    .select(
        "*",
        F.last("attr_key", ignorenulls=True).over(w).alias("last_known_attr_key"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
