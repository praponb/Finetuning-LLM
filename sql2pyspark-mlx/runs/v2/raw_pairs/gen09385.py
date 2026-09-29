from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("product_id").isin([6, 8]))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.lag("units", 1, 0).over(w).alias("prev_units"),
    )
)
