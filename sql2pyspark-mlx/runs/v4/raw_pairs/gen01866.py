from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 7, 9]))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.lag("units", 2).over(w).alias("prev_units"),
    )
)
