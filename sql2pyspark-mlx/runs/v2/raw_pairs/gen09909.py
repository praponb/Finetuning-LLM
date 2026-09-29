from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%oky%"))
    .select("id", "country", "rating", F.lead("rating", 1).over(w).alias("next_rating"))
    .orderBy("country")
)
