from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")

result = (
    df_store_sales
    .select("id", "sale_date", F.row_number().over(w).alias("sale_date_row"))
)
