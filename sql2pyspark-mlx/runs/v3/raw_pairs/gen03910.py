from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4]))
    .select(
        "id",
        "product_id",
        "store_id",
        "sale_date",
        "revenue",
        F.lag("revenue", 1).over(w).alias("prev_revenue"),
    )
)
