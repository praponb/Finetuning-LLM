from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy("cost")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "category",
        "cost",
        F.rank().over(w).alias("cost_rank"),
    )
    .orderBy(F.col("supplier_id").desc())
)
