from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("cost")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "cost",
        F.dense_rank().over(w).alias("cost_dense_rank"),
    )
)
