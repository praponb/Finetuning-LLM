from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id")
w3 = Window.orderBy(F.col("cost").asc_nulls_first())

result = (
    df_products
    .select(
        "*",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.lead("cost", 1).over(w2).alias("next_cost"),
        F.rank().over(w3).alias("cost_rank"),
    )
)
