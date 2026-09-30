from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date")

result = (
    df_products
    .filter(F.col("brand").isin(["Stark", "Acme", "Globex"]))
    .select(
        "id",
        "supplier_id",
        "launch_date",
        F.rank().over(w).alias("launch_date_rank"),
    )
)
