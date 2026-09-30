from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("cost", "id")

result = (
    df_products
    .filter(F.col("cost") <= 310)
    .select("id", "supplier_id", "cost", F.row_number().over(w).alias("cost_row"))
)
