from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 9, 10]))
    .select(
        "id",
        "store_id",
        "revenue",
        "product_id",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
    .select("id", "store_id", "revenue", "product_id")
)
