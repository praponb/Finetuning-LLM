from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
    .orderBy(F.col("id").desc(), F.col("launch_date").desc())
)
