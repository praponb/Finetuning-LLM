from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        "sale_date",
        "units",
        F.avg("revenue").over(w).alias("product_id_avg_revenue"),
        F.sum("units").over(w2).alias("running_units"),
    )
    .orderBy(F.col("product_id_avg_revenue").desc())
)
