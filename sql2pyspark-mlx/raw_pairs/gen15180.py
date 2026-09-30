from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id")
w2 = Window.partitionBy("supplier_id")

result = (
    df_products
    .filter(F.col("category") == "Home")
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 3).alias("cost_share"),
    )
)
