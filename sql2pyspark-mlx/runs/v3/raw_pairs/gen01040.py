from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date")
w2 = Window.partitionBy("supplier_id")

result = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "cost",
        F.rank().over(w).alias("rnk"),
        F.sum("cost").over(w2).alias("supplier_id_sum_cost"),
    )
)
