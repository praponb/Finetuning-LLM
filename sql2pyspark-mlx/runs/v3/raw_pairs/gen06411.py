from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost")
w2 = Window.partitionBy("brand")

result = (
    df_products
    .select(
        "id",
        "brand",
        "cost",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 3).alias("cost_share"),
    )
)
