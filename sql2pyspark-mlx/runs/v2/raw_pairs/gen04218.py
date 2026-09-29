from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([5, 8, 9]))
    .select(
        "id",
        "product_id",
        "revenue",
        "sale_date",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "product_id", "revenue", "sale_date")
)
