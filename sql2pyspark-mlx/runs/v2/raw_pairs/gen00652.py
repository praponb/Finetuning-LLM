from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc())

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        F.rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
)
