from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("price", "id")

result = (
    df_products
    .filter(F.length("product_name") >= 7)
    .select("id", "supplier_id", "price", F.row_number().over(w).alias("price_row"))
)
