from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("supplier_id") == 5)
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
)
