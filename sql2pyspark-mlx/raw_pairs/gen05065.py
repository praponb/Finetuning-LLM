from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "cost",
        F.min("cost").over(w).alias("supplier_id_min_cost"),
    )
    .orderBy(F.col("supplier_id").desc())
)
