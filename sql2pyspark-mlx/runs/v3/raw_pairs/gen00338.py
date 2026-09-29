from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy(F.col("cost").desc())

result = (
    df_products
    .filter(F.col("price") <= 1090)
    .select(
        "id",
        "category",
        "supplier_id",
        "cost",
        F.rank().over(w).alias("cost_rank"),
    )
)
