from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([4, 5, 11]))
    .select("id", "store_id", "sale_date", F.row_number().over(w).alias("row_num"))
)
