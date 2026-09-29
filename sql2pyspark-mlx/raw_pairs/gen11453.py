from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("units") <= 29)
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        F.first("revenue").over(w).alias("first_revenue"),
    )
)
