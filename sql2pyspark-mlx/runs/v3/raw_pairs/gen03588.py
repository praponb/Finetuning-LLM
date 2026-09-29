from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .filter(F.col("sale_date") >= "2024-02-25")
    .select(
        "id",
        "product_id",
        "revenue",
        "store_id",
        F.rank().over(w).alias("revenue_rank"),
        F.percent_rank().over(w2).alias("revenue_pct_rank"),
    )
)
