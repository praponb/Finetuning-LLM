from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "cost",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
    )
)
