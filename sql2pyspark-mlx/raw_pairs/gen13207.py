from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("launch_date"), "category").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "category",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
    .orderBy(F.col("cost").desc())
)
