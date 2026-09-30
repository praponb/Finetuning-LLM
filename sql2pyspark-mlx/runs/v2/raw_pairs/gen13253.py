from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("supplier_id").isin([1, 19, 32]))
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "price",
        F.lead("price", 1).over(w).alias("next_price"),
    )
)
