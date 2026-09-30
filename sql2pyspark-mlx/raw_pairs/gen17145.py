from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        "product_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 5)
    .select("id", "store_id", "revenue", "product_id")
)
