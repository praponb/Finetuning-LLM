from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .select(
        "id",
        "product_id",
        "revenue",
        F.avg("revenue").over(w).alias("product_id_avg_revenue"),
    )
)
