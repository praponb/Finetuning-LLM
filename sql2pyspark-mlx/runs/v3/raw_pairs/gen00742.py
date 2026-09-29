from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc())
w2 = Window.partitionBy("brand").orderBy("launch_date")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "cost",
        "brand",
        "launch_date",
        F.rank().over(w).alias("cost_rank"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
