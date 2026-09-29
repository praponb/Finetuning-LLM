from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Bangkok", "New York", "Singapore"]))
    .select("id", "city", "rating", "supplier_name", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .select("id", "city", "rating", "supplier_name")
)
