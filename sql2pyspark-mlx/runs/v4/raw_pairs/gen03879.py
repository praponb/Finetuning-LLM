from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating")

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select("id", "city", "rating", F.rank().over(w).alias("rnk"))
)
