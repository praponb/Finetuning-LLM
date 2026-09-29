from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1200)
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "store_id",
        "revenue",
        F.last("units").over(w).alias("last_units"),
        F.lead("revenue", 1, 0).over(w2).alias("next_revenue"),
    )
)
