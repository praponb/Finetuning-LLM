from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("price") >= 810)
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "cost",
        F.lead("cost", 1).over(w).alias("next_cost"),
    )
)
