from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("brand")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "product_name",
        "brand",
        "cost",
        F.last("product_name").over(w).alias("last_product_name"),
        F.sum("cost").over(w2).alias("brand_sum_cost"),
    )
)
