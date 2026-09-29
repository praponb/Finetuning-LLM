from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")

result = (
    df_store_sales
    .select("id", "product_id", "sale_date", F.row_number().over(w).alias("rn"))
    .orderBy("sale_date")
)
