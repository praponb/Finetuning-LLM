from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")

result = (
    df_inventory
    .filter(F.col("quantity").between(82, 169))
    .select("id", "quantity", F.row_number().over(w).alias("quantity_row"))
)
