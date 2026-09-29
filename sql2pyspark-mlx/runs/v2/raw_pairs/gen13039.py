from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.upper(F.trim("email")) == "IVY@GMAIL.COM")
    .select("id", "segment", "signup_date", F.ntile(2).over(w).alias("bucket"))
)
