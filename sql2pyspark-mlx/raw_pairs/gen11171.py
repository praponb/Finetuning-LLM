from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id")

result = (
    df_store_sales
    .select("id", "store_id", "sale_date", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "store_id", "sale_date")
)
