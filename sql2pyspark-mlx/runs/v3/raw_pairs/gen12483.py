from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_products
    .filter(F.col("price") > 790)
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "product_name",
        F.last("product_name").over(w).alias("last_product_name"),
    )
    .orderBy("product_name", F.col("launch_date").desc())
)
