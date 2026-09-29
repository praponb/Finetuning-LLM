from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy(F.date_trunc("week", "sale_date"), "product_id")

result = (
    df_store_sales
    .select(
        "*",
        F.count("units").over(w).alias("product_id_count_units"),
        F.min("units").over(w2).alias("sale_date_period_min_units"),
    )
)
