from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.upper(F.trim("supplier_name")) == "PACIFIC WHOLESALE")
    .select("id", "country", "rating", "city", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
)
