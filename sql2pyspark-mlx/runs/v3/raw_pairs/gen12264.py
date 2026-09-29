from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        F.percent_rank().over(w).alias("quantity_pct_rank"),
        F.dense_rank().over(w).alias("quantity_dense_rank"),
    )
)
