from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.orderBy(F.col("launch_date").desc())

result = (
    df_products
    .select(
        "id",
        "category",
        "cost",
        "launch_date",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
        F.dense_rank().over(w2).alias("launch_date_dense_rank"),
    )
)
