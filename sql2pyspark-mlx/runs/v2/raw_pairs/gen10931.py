from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") == 1)
    .orderBy(F.col("sale_date").desc())
)
