from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.col("attributes").getItem("gift").alias("gift"),
        F.col("attributes")["utm_source"].alias("utm_source"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.last("utm_source", ignorenulls=True).over(w_grp).alias("last_known_utm_source"),
        F.last("gift", ignorenulls=True).over(w_grp).alias("last_known_gift"),
        F.first("gift", ignorenulls=True).over(w_grp).alias("first_known_gift"),
    )
)
