from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("revenue")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        F.sum("revenue").over(w).alias("running_revenue"),
        F.rank().over(w2).alias("revenue_rank"),
    )
)
