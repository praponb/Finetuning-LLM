from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Glob%"))
    .select("id", "city", "rating", F.lead("rating", 1).over(w).alias("next_rating"))
)
