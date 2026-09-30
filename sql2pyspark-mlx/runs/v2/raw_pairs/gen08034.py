from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([2, 7, 8]))
    .select("id", "store_id", "units", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .orderBy("store_id", F.col("units").desc())
)
