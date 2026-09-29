from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("revenue")
w2 = Window.partitionBy(F.date_trunc("MONTH", "sale_date")).orderBy(F.col("sale_date").desc())

result = (
    df_store_sales
    .filter(F.col("revenue").between(1500, 3800))
    .select(
        "id",
        "product_id",
        "store_id",
        "revenue",
        "sale_date",
        F.rank().over(w).alias("revenue_rank"),
        F.rank().over(w2).alias("sale_date_rank"),
    )
    .orderBy(F.col("id").desc(), "revenue")
)
