from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("%Trad%"))
    .select("id", "city", "rating", F.ntile(10).over(w).alias("quartile"))
    .orderBy("rating")
)
